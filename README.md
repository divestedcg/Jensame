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
    - Found 10049077 files, hashed 802450 files, totalling 388119MB, and identified 691490 duplicates in 656920ms at 590MBps

Prebuilts
---------
- via CI: https://gitlab.com/divested/jensame/-/jobs/artifacts/master/browse?job=build

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
