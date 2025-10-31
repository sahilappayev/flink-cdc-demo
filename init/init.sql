CREATE TABLE cards (
                       card_id VARCHAR PRIMARY KEY,
                       auto_payment BOOLEAN
);

CREATE TABLE transactions (
                              transaction_id VARCHAR PRIMARY KEY,
                              card_id VARCHAR,
                              amount DECIMAL
);

-- Test data
INSERT INTO cards VALUES ('C123', true),
                         ('C124', false),
                         ('C125', false);
INSERT INTO transactions VALUES ('T1', 'C123', 100),
                                ('T2', 'C124', 157),
                                ('T3', 'C125', 145),
                                ('T4', 'C126', 200);
