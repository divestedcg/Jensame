jensame
=======

A high-performance duplicate file finder.
Meant for use with btrfs/xfs + duperemove utility.

Use
---
- gradle assemble
- java -jar -XX:+UseShenandoahGC jensame.jar $fdupesOutput $pathsToRecurse
- cat $fdupesOutput | sudo duperemove -d -b32768 --fdupes -v
- Example output:
    - Found 10019289 files, hashed 792980 files, totalling 380287MB, and identified 684148 duplicates in 677020ms at 561MBps

TODO
----
- User defined thread count
- Default thread count determined by storage medium
- Alternate hash algorithms

Credits
-------
- Zero-Allocation Hashing (Apache-2.0), https://github.com/OpenHFT/Zero-Allocation-Hashing

Donate
-------
- https://divested.dev/donate
