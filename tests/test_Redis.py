import redis


r = redis.StrictRedis(host='localhost', port=6379, db=0)

#r.set('foo', 'bar')
#print r.get('foo')

#On the real program we will have to create a leaderboars fora each n_index from tuples with the format:
# (n_index, lab, count)
#We will use SORTED SET struct to store data
#Leaderboard for n_index = 1.5
r.zadd('leaderboard:1.5', 20, 'Visionworks')
r.zadd('leaderboard:1.5', 10, 'VSP')
r.zadd('leaderboard:1.5', 40, 'LOA')
r.zadd('leaderboard:1.5', 3, 'Szajna')
r.zadd('leaderboard:1.5', 9, 'Vitolen')

print r.zrevrange('leaderboard:1.5', 0, 10, withscores=True)
