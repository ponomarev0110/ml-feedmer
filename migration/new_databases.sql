CREATE SCHEMA IF NOT EXISTS public;

-- Table: public.users

CREATE TABLE IF NOT EXISTS public.users
(
    userid integer NOT NULL UNIQUE,
    formaladdr text,
    payway text,
    CONSTRAINT users_pk PRIMARY KEY (userid)
);

-- Table: public.building_types

CREATE TABLE IF NOT EXISTS public.building_types
(
    formaladdr text NOT NULL,
    building_type text,
    CONSTRAINT building_types_pkey PRIMARY KEY (formaladdr)
);

-- Table: public.user_history

CREATE TABLE IF NOT EXISTS public.user_history
(
    userid integer NOT NULL,
    strdate TIMESTAMP NOT NULL,
    hasordered boolean,
    price numeric,
    CONSTRAINT user_history_pkey PRIMARY KEY (userid, strdate)
);

-- Table: public.user_statistics

CREATE TABLE IF NOT EXISTS public.user_statistics
(
    userid integer NOT NULL,
    strdate TIMESTAMP,
    average_order_price_60 numeric,
    average_order_count_60 bigint,
    average_order_price_14 numeric,
    average_order_count_14 bigint,
    average_order_price_7 numeric,
    average_order_count_7 bigint,
    order_frequency numeric,
    days_since_last_order integer,
    CONSTRAINT user_statistics_pkey PRIMARY KEY (userid, strdate)
);

-- Table: public.weather

CREATE TABLE IF NOT EXISTS public.weather
(
    formaladdr text NOT NULL,
    strdate TIMESTAMP NOT NULL,
    latitude numeric,
    longitude numeric,
    apparenttemperaturehigh numeric,
    apparenttemperaturelow numeric,
    temperaturelow numeric,
    temperaturehigh numeric,
    cloudcover numeric,
    humidity numeric,
    precipintensity numeric,
    precipprobability numeric,
    preciptype text,
    pressure numeric,
    windspeed numeric,
    windbearing numeric,
    moonphase numeric,
    CONSTRAINT weather_pkey PRIMARY KEY (formaladdr, strdate)
);