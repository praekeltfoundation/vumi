DROP TABLE IF EXISTS ikhwezi_quiz;

CREATE TABLE ikhwezi_quiz (
    id SERIAL PRIMARY KEY,
    msisdn varchar UNIQUE NOT NULL,
    provider varchar,
    sessions integer,
    msisdn_timestamp timestamp,
    question1 integer,
    question2 integer,
    question3 integer,
    question4 integer,
    question5 integer,
    question6 integer,
    question7 integer,
    question8 integer,
    question9 integer,
    question10 integer,
    question1_correct integer,
    question2_correct integer,
    question3_correct integer,
    question4_correct integer,
    question5_correct integer,
    question6_correct integer,
    question7_correct integer,
    question8_correct integer,
    question9_correct integer,
    question10_correct integer,
    question1_timestamp timestamp,
    question2_timestamp timestamp,
    question3_timestamp timestamp,
    question4_timestamp timestamp,
    question5_timestamp timestamp,
    question6_timestamp timestamp,
    question7_timestamp timestamp,
    question8_timestamp timestamp,
    question9_timestamp timestamp,
    question10_timestamp timestamp,
    demographic1 integer,
    demographic2 integer,
    demographic3 integer,
    demographic4 integer,
    demographic1_timestamp timestamp,
    demographic2_timestamp timestamp,
    demographic3_timestamp timestamp,
    demographic4_timestamp timestamp,
    remaining_questions varchar,
    original_questions varchar,
    winner varchar,
    winner_timestamp timestamp
);

CREATE INDEX idx_ikhwezi_quiz_provider ON ikhwezi_quiz (provider);
CREATE INDEX idx_ikhwezi_quiz_attempts ON ikhwezi_quiz (sessions);
CREATE INDEX idx_ikhwezi_quiz_msisdn_timestamp ON ikhwezi_quiz (msisdn_timestamp);
CREATE INDEX idx_ikhwezi_quiz_question1 ON ikhwezi_quiz (question1);
CREATE INDEX idx_ikhwezi_quiz_question2 ON ikhwezi_quiz (question2);
CREATE INDEX idx_ikhwezi_quiz_question3 ON ikhwezi_quiz (question3);
CREATE INDEX idx_ikhwezi_quiz_question4 ON ikhwezi_quiz (question4);
CREATE INDEX idx_ikhwezi_quiz_question5 ON ikhwezi_quiz (question5);
CREATE INDEX idx_ikhwezi_quiz_question6 ON ikhwezi_quiz (question6);
CREATE INDEX idx_ikhwezi_quiz_question7 ON ikhwezi_quiz (question7);
CREATE INDEX idx_ikhwezi_quiz_question8 ON ikhwezi_quiz (question8);
CREATE INDEX idx_ikhwezi_quiz_question9 ON ikhwezi_quiz (question9);
CREATE INDEX idx_ikhwezi_quiz_question10 ON ikhwezi_quiz (question10);
CREATE INDEX idx_ikhwezi_quiz_question1_correct ON ikhwezi_quiz (question1);
CREATE INDEX idx_ikhwezi_quiz_question2_correct ON ikhwezi_quiz (question2);
CREATE INDEX idx_ikhwezi_quiz_question3_correct ON ikhwezi_quiz (question3);
CREATE INDEX idx_ikhwezi_quiz_question4_correct ON ikhwezi_quiz (question4);
CREATE INDEX idx_ikhwezi_quiz_question5_correct ON ikhwezi_quiz (question5);
CREATE INDEX idx_ikhwezi_quiz_question6_correct ON ikhwezi_quiz (question6);
CREATE INDEX idx_ikhwezi_quiz_question7_correct ON ikhwezi_quiz (question7);
CREATE INDEX idx_ikhwezi_quiz_question8_correct ON ikhwezi_quiz (question8);
CREATE INDEX idx_ikhwezi_quiz_question9_correct ON ikhwezi_quiz (question9);
CREATE INDEX idx_ikhwezi_quiz_question10_correct ON ikhwezi_quiz (question10);
CREATE INDEX idx_ikhwezi_quiz_demographic1 ON ikhwezi_quiz (demographic1);
CREATE INDEX idx_ikhwezi_quiz_demographic2 ON ikhwezi_quiz (demographic2);
CREATE INDEX idx_ikhwezi_quiz_demographic3 ON ikhwezi_quiz (demographic3);
CREATE INDEX idx_ikhwezi_quiz_demographic4 ON ikhwezi_quiz (demographic4);
CREATE INDEX idx_ikhwezi_quiz_winner ON ikhwezi_quiz (winner);


