jensame
=======

A high-performance duplicate file finder.

Use
---
- gradle assemble
- java -jar jensame.jar $pathToRecurse $fdupesOutput
- cat $fdupesOutput | sudo duperemove -d -b4096 --fdupes -v

Credits
-------
- Zero-Allocation Hashing (Apache-2.0), https://github.com/OpenHFT/Zero-Allocation-Hashing

Donate
-------
- https://divested.dev/donate
