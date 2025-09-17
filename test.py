from datetime import datetime
import pytz
s = 1756905662
timezone = pytz.timezone('CET')
dt = datetime.fromtimestamp(s, tz=timezone).replace(tzinfo=None)

print(dt)
