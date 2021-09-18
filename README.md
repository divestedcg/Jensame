jensame
=======

A high-performance duplicate file finder.
Meant for use with btrfs/xfs + duperemove utility.

Use
---
- gradle assemble
- java -jar jensame.jar $fdupesOutput $pathsToRecurse
- cat $fdupesOutput | sudo duperemove -d -b32768 --fdupes -v
- Example output:
    - Hashed 839625 files, totalling 835933MB, and identified 684150 duplicates in 1617162ms at 516MBps

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
