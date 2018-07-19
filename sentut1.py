import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader.data as web

style.use('ggplot')

start = dt.datetime(2015, 1, 1)
end = dt.datetime.now()

df = web.DataReader("TSLA", 'morningstar', start, end)

df.reset_index(inplace=True)
df.set_index("Date", inplace=True)
df = df.drop("Symbol", axis=1)

df.to_csv('TSLA.csv')

df = pd.read_csv('tsla.csv', parse_dates=True, index_col=0)

df['Close'].plot()
plt.show()


# print(df.tail())
