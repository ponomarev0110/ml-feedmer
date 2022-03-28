CREATE TABLE IF NOT EXISTS predictions(
    userid INT NOT NULL,
    strdate TEXT NOT NULL,
    orderProbability NUMERIC NOT NULL,
    PRIMARY KEY (userid, strdate)
);