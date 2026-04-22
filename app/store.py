import asyncio

store = {}
expiry = {}

def set_key(key, value, px=None):
    store[key] = value

    if px:
        expiry[key] = asyncio.get_running_loop().time() + (px / 1000)

def get_key(key):
    if key in expiry:
        if asyncio.get_running_loop().time() > expiry[key]:
            store.pop(key, None)
            expiry.pop(key, None)
            return None

    return store.get(key)
