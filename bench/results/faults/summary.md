| target | seeds | faults | acked ids | contract violations | dups beyond 1/reader | zero-ack s / load s |
|---|---|---|---|---|---|---|
| kafka | 3 | 37 | 81450 | 0 | 200 | 37 / 366 |
| redpanda | 3 | 26 | 95370 | 0 | 20 | 10 / 370 |
| replog | 3 | 39 | 74545 | 0 | 0 | 99 / 377 |

Leader faults — p50 / p90 / max in ms (metadata-visible leader change; largest ack gap in the fault window):

| target | fault | leader moved | largest ack gap |
|---|---|---|---|
| kafka | kill | 3062 / 4431 / 4747 (n=10) | 3528 / 5241 / 5418 (n=10) |
| kafka | pause | 2073 / 4152 / 4761 (n=7) | 2041 / 5060 / 5091 (n=7) |
| kafka | isolate | 3199 / 4449 / 4877 (n=10) | 4591 / 5926 / 6981 (n=11) |
| redpanda | kill | 3402 / 3454 / 3454 (n=2) | 4072 / 4290 / 4290 (n=2) |
| redpanda | pause | 4683 / 4683 / 4683 (n=1) | 4036 / 4036 / 4036 (n=1) |
| redpanda | isolate | 4910 / 6059 / 6613 (n=8) | 4836 / 6973 / 8809 (n=10) |
| replog | kill | 866 / 1182 / 1182 (n=13) | 7233 / 7236 / 7236 (n=13) |
| replog | pause | 2054 / 2061 / 2061 (n=8) | 5857 / 19990 / 20765 (n=8) |
| replog | isolate | 1098 / 1997 / 1997 (n=13) | 6602 / 6743 / 6772 (n=13) |

Follower faults — largest ack gap in the fault window (the stall a follower's death costs the leader):

| target | fault | largest ack gap |
|---|---|---|
| kafka | kill | 1086 / 3339 / 4262 (n=18) |
| kafka | pause | 1029 / 1471 / 4315 (n=9) |
| kafka | isolate | 997 / 1627 / 4955 (n=19) |
| redpanda | kill | 41 / 58 / 64 (n=16) |
| redpanda | pause | 43 / 54 / 54 (n=11) |
| redpanda | isolate | 44 / 50 / 56 (n=12) |
| replog | kill | 1659 / 1775 / 1777 (n=13) |
| replog | pause | 1696 / 1752 / 1753 (n=8) |
| replog | isolate | 1657 / 1768 / 1780 (n=23) |

Controls (same seed, isolate-only schedule; only the ack level differs):

| target | run | acked | acked ids lost | violations |
|---|---|---|---|---|
| kafka | control-acks1 | 20610 | 2515 | 1 |
| kafka | control-acksall | 15030 | 0 | 0 |
| kafka | baseline | 8775 | 0 | 0 |
| redpanda | control-acks1 | 20105 | 1185 | 1 |
| redpanda | control-acksall | 19280 | 0 | 0 |
| redpanda | baseline | 8460 | 0 | 0 |
| replog | control-acks1 | 24085 | 5875 | 1 |
| replog | control-acksall | 15280 | 0 | 0 |
| replog | baseline | 10620 | 0 | 0 |
