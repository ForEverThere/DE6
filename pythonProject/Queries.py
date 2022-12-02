insSTAGE_TABLE = (f"""INSERT INTO STAGE_TABLE SELECT 
       _id,
       IOS_App_Id,
       Title,
       Developer_Name,
       Developer_IOS_Id,
       IOS_Store_Url,
       Seller_Official_Website,
       Age_Rating,
       Total_Average_Rating,
       Total_Number_of_Ratings,
       Average_Rating_For_Version,
       Number_of_Ratings_For_Version,
       Original_Release_Date,
       Current_Version_Release_Date,
       Price_USD,
       Primary_Genre,
       All_Genres,
       Languages,
       Description 
       FROM RAW_STREAM""")

insMASTER_TABLE = (f"""INSERT INTO MASTER_TABLE SELECT 
       _id,
       IOS_App_Id,
       Title,
       Developer_Name,
       Developer_IOS_Id,
       IOS_Store_Url,
       Seller_Official_Website,
       Age_Rating,
       Total_Average_Rating,
       Total_Number_of_Ratings,
       Average_Rating_For_Version,
       Number_of_Ratings_For_Version,
       Original_Release_Date,
       Current_Version_Release_Date,
       Price_USD,
       Primary_Genre,
       All_Genres,
       Languages,
       Description 
       FROM STAGE_STREAM""")

sql_query = """
        select * from information_schema.tables
        where table_name = upper('raw_table')
    """

insert_query = """
    INSERT INTO RAW_TABLE(
                    _id,
                    IOS_App_Id,
                    Title,
                    Developer_Name,
                    Developer_IOS_Id,
                    IOS_Store_Url,
                    Seller_Official_Website,
                    Age_Rating,
                    Total_Average_Rating,
                    Total_Number_of_Ratings,
                    Average_Rating_For_Version,
                    Number_of_Ratings_For_Version,
                    Original_Release_Date,
                    Current_Version_Release_Date,
                    Price_USD,
                    Primary_Genre,
                    All_Genres,
                    Languages,
                    Description)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""