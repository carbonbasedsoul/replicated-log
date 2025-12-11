# Replicated log, iteration 3

`tests/` - test suite generated with Claude Opus 4.5 to test the solution in a compact and comprehensive way.


## 1. Self-check acceptance

> Self-check acceptance test:
Start M + S1  
send (Msg1, W=1) - Ok  
send (Msg2, W=2) - Ok  
send (Msg3, W=3) - Wait  
send (Msg4, W=1) - Ok  
Start S2  
Check messages on S2 - [Msg1, Msg2, Msg3, Msg4]  


```zsh
❯ uv run tests/test_acceptance.py
Acceptance Test
==================================================
Setup: master + secondary-1
Msg1 (w=1)
Msg2 (w=2)
Msg3 (w=3) – expect block
Msg4 (w=1) – should be fast
  ✓ completed in 0.03s
Starting secondary-2
Waiting for Msg3 to finish
Waiting for secondary-2 catch-up
  ✓ secondary-2 messages: ['Msg1', 'Msg2', 'Msg3', 'Msg4']

✓ ACCEPTANCE TEST PASSED
```


## 2. Deduplication

>Test deduplication with direct injection.
>- Send message through master
>- Inject duplicate directly to secondary
>- Verify message appears exactly once


```zsh
❯ uv run tests/test_deduplication.py
Deduplication Test
==================================================
Starting containers...
Sending msg1 through master (w=2)...
Injecting duplicate to secondary-1...
Checking logs for duplicate detection...
  ✓ Duplicate detected in logs
Verifying message count...
  ✓ Exactly 1 message on secondary-1

✓ DEDUPLICATION TEST PASSED
```


## 3. Total order

>Test total order with direct injection.
>- Send msg1, msg2, msg3 through master
>- Inject msg5 directly to secondary (creating gap)
>- Verify secondary hides msg5 until msg4 arrives


```zsh
❮ uv run test_total_order.py
Total Order Test
==================================================
Starting containers...
Sending msg1, msg2, msg3...
Injecting msg5 directly (creating gap at msg4)...
Checking secondary-2 hides msg5...
  ✓ Secondary-2 shows: ['msg1', 'msg2', 'msg3'] (msg5 hidden)
Sending msg4 (filling gap)...
Checking secondary-2 now shows all messages...
  ✓ Secondary-2 shows: ['msg1', 'msg2', 'msg3', 'msg4', 'msg5'] (gap filled)

✓ TOTAL ORDER TEST PASSED
```
