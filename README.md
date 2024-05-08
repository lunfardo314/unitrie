# unitrie

Universal package which implements tries (committed radix tries) with the configurable cryptographic commitment models.
Commitment models can use any hash function (sha.., blake2, zk-friendly) or polynomial (KZG) commitments.

Used to implement sparse Merkle trees, Patricia tree (hexary) and verkle trees (with KZG commitments). 
