1. use -----  python "data_republisher.py"     to run data_republisher.py this document will directly use the function in data_gatherer.py to fatch the data from fuel API.
2. use another terminal to run ----     python "data_ingester.py"
3. use another terminal to run ----     streamlit run dashboard_visualization_database.py    to get the dashboard from PostgresSQL database.
                                                                                                                        the database's psw and user name are 
                                                                                                                        host = "localhost"
                                                                                                                        user = "postgres"
                                                                                                                        psw = "123456789"
4. or use another terminal to run ----     streamlit run dashboard_visualization_csv.py    to get the dashboard from fuel_prices.csv document.
