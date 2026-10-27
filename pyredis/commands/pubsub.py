"""
Pub/Sub command handlers: SUBSCRIBE, UNSUBSCRIBE, PUBLISH.
"""
from typing import Set

from pyredis.protocol import BulkString, Integer
from .base import registry


@registry.register("SUBSCRIBE", is_mutating=False, min_args=1)
def cmd_subscribe(ctx, *channels: bytes):
    ctx.connection.is_pubsub = True
    responses = []

    for chan in channels:
        ctx.connection.subscribed_channels.add(chan)
        subscribers = ctx.server.pubsub_channels.setdefault(chan, set())
        subscribers.add(ctx.connection)

        # Response frame: ["subscribe", channel_name, num_channels_subscribed]
        frame = [
            BulkString(b"subscribe"),
            BulkString(chan),
            Integer(len(ctx.connection.subscribed_channels)),
        ]
        responses.append(frame)

    return responses


@registry.register("UNSUBSCRIBE", is_mutating=False, min_args=0)
def cmd_unsubscribe(ctx, *channels: bytes):
    target_channels = channels if channels else list(ctx.connection.subscribed_channels)
    responses = []

    for chan in target_channels:
        if chan in ctx.connection.subscribed_channels:
            ctx.connection.subscribed_channels.remove(chan)
            subscribers = ctx.server.pubsub_channels.get(chan)
            if subscribers and ctx.connection in subscribers:
                subscribers.remove(ctx.connection)
                if not subscribers:
                    del ctx.server.pubsub_channels[chan]

        frame = [
            BulkString(b"unsubscribe"),
            BulkString(chan),
            Integer(len(ctx.connection.subscribed_channels)),
        ]
        responses.append(frame)

    if not ctx.connection.subscribed_channels:
        ctx.connection.is_pubsub = False

    return responses


@registry.register("PUBLISH", is_mutating=False, min_args=2, max_args=2)
def cmd_publish(ctx, channel: bytes, message: bytes):
    subscribers = ctx.server.pubsub_channels.get(channel)
    if not subscribers:
        return Integer(0)

    delivered = 0
    msg_frame = [
        BulkString(b"message"),
        BulkString(channel),
        BulkString(message),
    ]

    for client in list(subscribers):
        try:
            client.send_frame(msg_frame)
            delivered += 1
        except Exception:
            subscribers.discard(client)

    return Integer(delivered)
