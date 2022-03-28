CREATE TABLE IF NOT EXISTS predictions(
    userid INT NOT NULL,
    strdate TEXT NOT NULL,
    orderProbability NUMERIC,
    PRIMARY KEY (userid, strdate)
);