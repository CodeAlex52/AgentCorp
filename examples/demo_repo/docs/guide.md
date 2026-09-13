# Guide

`TaskQueue` is a bounded FIFO. Push raises `BufferError` when full; pop is LIFO-free.
