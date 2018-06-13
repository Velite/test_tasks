import json
import csv

from peewee import *
from playhouse.sqlite_ext import SqliteExtDatabase


def parse_afinn_file(filename):
	with open(filename, "r", encoding = "utf-8") as f:
		lines = csv.reader(f, delimiter = "\t")
		return dict((k, int(v)) for k, v in lines)


def parse_tweets_file(filename):
	with open(filename, "r", encoding = "utf-8") as f:
		for line in f:
			j = json.loads(line)
			user = j.get("user")
			if user is not None:
				t_text = j.get("text")
				t_date = j.get("created_at")
				t_lang = j.get("lang")
				u_name = user.get("name")
				u_location = user.get("location")
				t_url = ""  # ??
				t_ccode = ""  # ??
				yield dict(
					name = u_name,
					tweet_text = t_text,
					country_code = t_ccode,
					display_url = t_url,
					lang = t_lang,
					created_at = t_date,
					location = u_location
				)


def get_tweet_value(text, costs):
	result = 0
	if text is not "":
		text_words = list(map(lambda s: "".join(c for c in s if c.isalnum()), text.split()))
		# y = lambda x: costs.get(x, 0) if x in costs.keys() else 0
		# z = reduce(lambda x, v: x + y(v), text_words, 0)
		for w in text_words:
			if w in costs.keys():
				result += costs.get(w, 0)
	return result


def chunk(it, n):
	global xs
	try:
		while True:
			xs = []
			for _ in range(n):
				xs.append(next(it))
			yield xs
	except StopIteration:
		yield xs


db = SqliteExtDatabase("mts_task2.db")


class BaseModel(Model):
	class Meta:
		database = db


class Tweet(BaseModel):
	name = CharField()
	tweet_text = TextField()
	country_code = CharField()
	display_url = CharField()
	lang = CharField()
	created_at = DateTimeField()
	location = CharField()
	tweet_sentiment = IntegerField(default = 0)

	class Meta:
		primary_key = False


db.connect()
db.drop_table(Tweet, fail_silently = True)
db.create_table(Tweet, safe = True)

items = parse_tweets_file("three_minutes_tweets.json.txt")
with db.atomic():
	for i in chunk(items, 100):
		Tweet.insert_many(i).execute()

db.close()

word_costs = parse_afinn_file("AFINN-111.txt")

db.connect()

all_tweets = Tweet.select()
for tweet in all_tweets:
	tweet_sentiment = get_tweet_value(tweet.tweet_text, word_costs)
	if tweet_sentiment != 0:
		query = Tweet.update(tweet_sentiment = tweet_sentiment)
		# query.execute()
		# tweet.save()
db.commit()
db.close()
