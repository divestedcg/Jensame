jensame
=======

A high-performance duplicate file finder.
Meant for use with btrfs/xfs + duperemove utility.

Use
---
- gradle assemble
- java -jar jensame.jar $pathToRecurse $fdupesOutput
- cat $fdupesOutput | sudo duperemove -d -b4096 --fdupes -v

TODO
----
- User defined thread count
- Default thread count determined by storage medium
- Sorted by file size optimization?
- Alternate hash algorithms

Credits
-------
- Zero-Allocation Hashing (Apache-2.0), https://github.com/OpenHFT/Zero-Allocation-Hashing

Donate
-------
- https://divested.dev/donate
