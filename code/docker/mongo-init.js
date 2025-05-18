db.createUser({
    user: "mongo_user",
    pwd: "mongo_password",
    roles: [{ role: "readWrite", db: "twitter_db" }]
});
db.createCollection("tweets");