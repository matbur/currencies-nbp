CREATE TABLE
    IF NOT EXISTS "prices" (
    "created_at" TIMESTAMP NOT NULL DEFAULT NOW(),
    "date" DATE NOT NULL,
    "currency" VARCHAR(3) NOT NULL,
    "price" NUMERIC NOT NULL,
    CONSTRAINT "prices_pkey" PRIMARY KEY ("currency", "date")
    );
