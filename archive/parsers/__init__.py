"""Parsers package"""
from .base_parser import BaseParser, ParsedSignal
from .parser_channel1 import Channel1Parser
from .parser_channel2 import Channel2Parser
from .parser_channel3 import Channel3Parser
from .claude_fallback import ClaudeFallbackParser

__all__ = [
    'BaseParser', 'ParsedSignal',
    'Channel1Parser', 'Channel2Parser', 'Channel3Parser',
    'ClaudeFallbackParser'
]
