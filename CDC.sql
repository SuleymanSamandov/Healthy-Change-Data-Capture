CREATE PROCEDURE [dbo].[CHANGE_DATA_CAPTURE]
AS
BEGIN
    DECLARE @PACK_NAME NVARCHAR(25) = 'cdc_package_name';
    DECLARE @TABLE_NAME NVARCHAR(25) = 'source_table';
    DECLARE @LAST_SUCCESSFUL_DATE DATETIME;
	DECLARE @PROCESS_END_TIME DATETIME = GETDATE();

    IF OBJECT_ID('temp_table', 'U') IS NOT NULL
    BEGIN
        DROP TABLE temp_table;
    END

	IF NOT EXISTS(SELECT 1 FROM log_table WHERE package_name=@PACK_NAME AND table_name=@TABLE_NAME)
		BEGIN 
			SET @LAST_SUCCESSFUL_DATE='1900-01-01'
		END
	ELSE
		BEGIN
			SELECT @LAST_SUCCESSFUL_DATE = MAX(LAST_SUCCESSFUL_DATE)
			FROM log_table 
			WHERE PACKAGE_NAME = @PACK_NAME AND TABLE_NAME = @TABLE_NAME;
		END

    CREATE TABLE temp_table
    (
        id INT,
        product_name NVARCHAR(25),
        product_category NVARCHAR(25),
        modify_date DATETIME
    );

    INSERT INTO temp_table (id, product_name, product_category, modify_date)
    SELECT id, product_name, product_category, modify_date
    FROM source_data 
    WHERE modify_date > @LAST_SUCCESSFUL_DATE;

   MERGE INTO target_data AS TRG
	USING temp_table AS SRC
	ON TRG.id = SRC.id
	WHEN MATCHED AND
		(SRC.product_name <> TRG.product_name OR 
		 SRC.product_category <> TRG.product_category OR 
		 SRC.modify_date <> TRG.modify_date)
	THEN
		UPDATE SET 
			TRG.product_name = SRC.product_name,
			TRG.product_category = SRC.product_category,
			TRG.modify_date = SRC.modify_date
	WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (id, product_name, product_category, modify_date)
		VALUES (SRC.id, SRC.product_name, SRC.product_category, SRC.modify_date);


    IF NOT EXISTS (SELECT 1 FROM log_table WHERE package_name = @PACK_NAME AND table_name = @TABLE_NAME)
    BEGIN
        INSERT INTO log_table (package_name, table_name, last_successful_date)
        VALUES (@PACK_NAME, @TABLE_NAME, @PROCESS_END_TIME);
    END
    ELSE
    BEGIN
        UPDATE log_table
        SET last_successful_date = @PROCESS_END_TIME
        WHERE package_name = @PACK_NAME AND table_name = @TABLE_NAME;
    END
END;
