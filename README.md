# TinkoffAPI-Historical-Data-Parser
This simple tool allows tinkoff-api users to download historical data on MOEX shares without any need to use moexapi to to parse data from investing.com and other hubs.
It makes parsing historical data from MOEX even more convenient, as is created different folders for each share to save .xlsx files in it. Then, you can use some tools provided to make a fully ready-to-work pandas dataset for your future analysis. Singlethreaded parser has to be used to update or download initially datasets for one or some very small number of shares you want to work with, as it may appear to be more secure and safe. Multithreaded one should be used in any other case. 

# Getting started:
## TinkoffAPI 
1) Visit https://www.tinkoff.ru/invest/settings/
2) Scroll down and find "Создать токен" button. Give it a full access. Copy it

## Your PC
1) Create a directory and 2 .ipynb or .py files in it.
2) Download this repo and copy the code to your files in your IDE. I recommend to use VSCode with an Anaconda Base to manage your files correctly.

## Parser
1) Go to the file where singlethreaded parser was previously implemented.
2) Insert your TinkoffAPI token
3) Insert the path to your directory 
4) Go to the file where multilethreaded parser was previously implemented.
5) Insert the same data as above
6) Insert the year you want to download your data from for all shares
7) Insert the shares you want to work with into a dict wish_instruments_tickers. Make sure you insert the correct tickers of your shares.


   



