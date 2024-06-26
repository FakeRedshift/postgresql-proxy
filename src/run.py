from postgres_proxy import proxy
import os

if __name__ == '__main__':
    path = os.path.dirname(os.path.realpath(__file__))
    proxy.run(path)
