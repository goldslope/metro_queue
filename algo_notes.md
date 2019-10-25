## Reference Counting
- P Norm, C Norm = V --> P XCHG t+2 -> t, C READS t+2, C WRITES ref+1
- C Norm, P Norm = V --> C READS t, C WAITS, P XCHG t+2 -> t, C READS t+2, C WRITES ref+1
- P ABAd, C Norm = I --> P XCHG t+1 -> t, C READS t+1, C WRITES t+2, C WRITES ref+1
- C Norm, P ABAd = I --> C READS t, C WAITS, P XCHG t+1 -> t, C READS t+1, C WRITES t+2, C WRITES ref+1
- P Norm, C ABAd = V --> P XCHG t+2 -> t, C XCHG t+2 -> t+2, C WRITES ref+1
- C ABAd, P Norm = I --> C XCHG t+2 -> t, P XCHG t+2 -> t+2, P WRITES ref+1
- P ABAd, C ABAd = I --> P XCHG t+1 -> t, C XCHG t+2 -> t+1, C WRITES ref+1
- C ABAd, P ABAd = I --> C XCHG t+2 -> t, P XCHG t+1 -> t+2, P WRITES t+2, P WRITES ref+1

## Designated Releaser 
- P Norm, C Norm = V --> P XCHG t+1 -> t, C READS t+1, C WRITES t+2
- C Norm, P Norm = V --> C READS t, C WAITS, P XCHG t+1 -> t, C READS t+1, C WRITES t+2
- P ABAd, C Norm = I --> P XCHG t+2 -> t, C READS ANY
- C Norm, P ABAd = I --> C READS t, C WAITS, P XCHG t+2 -> t, C READS ANY
- P Norm, C ABAd = V --> P XCHG t+1 -> t, C CMPXCHG t,t+1 -> t+1, C WRITES t+2
- C ABAd, P Norm = I --> C CMPXCHG t,t+1 -> t, P XCHG t+1 -> t+1, P WRITES t+2
- P ABAd, C ABAd = I --> P XCHG t+2 -> t, C CMPXCHG t,t+1 -> ANY
- C ABAd, P ABAd = I --> C CMPXCHG t,t+1 -> t, P XCHG t+2 -> t+1
