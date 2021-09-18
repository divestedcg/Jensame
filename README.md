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
    - Found 839625 files, hashed 792967 files, totalling 380284MB, and identified 684150 duplicates in 730470ms at 520MBps

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
