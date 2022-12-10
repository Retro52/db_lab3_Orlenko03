DO $$
    DECLARE
     thread_idx   Threads.thread_id%TYPE;
     thread_num Threads.thread_count%TYPE;

    BEGIN
     thread_idx := 148;
     thread_num := 7;
     FOR counter IN 1..20
         LOOP
            INSERT INTO Threads (thread_id, thread_count)
             VALUES (thread_idx, thread_num + counter);
         END LOOP;
    END;
    $$