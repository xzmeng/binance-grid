#%%
from pysondb import db
from tinydb import TinyDB

p = db.getDb('filled_orders.json')
t = TinyDB('db.json').table('filled')
for item in p.getAll():
    del item['id']
    t.insert(item)
