DO $$
DECLARE
   lmt        INT := 10000;
   lkz        VARCHAR(50) := NULL;
   divisor    INT := 1;
   modresult  INT := 0;
BEGIN
   CALL smartdatastagdb.ac_splithausnr(lkz, lmt, divisor, modresult);
   RAISE NOTICE 'lmt = %, lkz = %, divisor = %, modresult = %', lmt, lkz, divisor, modresult;

END $$;