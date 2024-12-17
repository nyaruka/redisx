v0.9.0 (2024-12-17)
-------------------------
 * Test against redis 7 and valkey 8 too
 * Update deps

v0.8.1 (2024-05-21)
-------------------------
 * Use std library errors

v0.8.0 (2024-03-13)
-------------------------
 * Update assertredis asserts to take a connection instead of a pool

v0.7.0 (2024-03-12)
-------------------------
 * Improve asserts

v0.6.4 (2024-01-30)
-------------------------
 * Fix again

v0.6.3 (2024-01-30)
-------------------------
 * Fix assertrange.LRange expected type

v0.6.2 (2024-01-30)
-------------------------
 * Add assertredis.LRange

v0.6.1 (2024-01-30)
-------------------------
 * Add assertredis.ZScore

v0.6.0 (2024-01-12)
-------------------------
 * Add NewPool helper
 * Update deps

v0.5.0 (2023-09-01)
-------------------------
 * Test on go 1.21
 * Tweak IntervalHash.Del and IntervalSet.Rem to support removing multiple keys like HDEL and SREM
 * Tweak method naming to be closer to the underlying redis commands
 * Implement MGET for interval hashes

v0.4.0 (2023-08-31)
-------------------------
 * Improve redis asserts so they return the equality result
 * Add pattern arg to assertredis.Keys
 * Add assertredis.HGet 
 * Properly support sub minute interval times

v0.3.1 (2023-05-24)
-------------------------
 * Fix assertredis.SIsMember

v0.3.0 (2023-05-24)
-------------------------
 * Update dependencies
 * Add assertredis.SIsMember

v0.2.2
----------
 * Add assertredis.HLen and LLen

v0.2.1
----------
 * Switch from retracted redigo release to latest

v0.2.0
----------
 * Add assertredis.ZCard and assertredis.SCard

v0.1.0
----------
* Initial revision

