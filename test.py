str = '"This should work for you" I said'
fecha = '2015/09/29'
print(fecha)

from datetime import datetime
# v6eZz&sa6M!sDwW

isodate = 'ISODate(' + datetime.now().isoformat()[:23] + 'Z' + ')'
print(datetime.now())
print(datetime.now().isoformat()[:23])
print(isodate)


datetime_object = datetime.strptime(fecha, '%Y/%m/%d')
isodate = 'ISODate(' + datetime_object.isoformat() + '.000Z' + ')'
print(datetime_object)
print(isodate)
