a=10

def test_func():
    a=15
    def inner_func():
        a=20
        print(a)
    inner_func()

test_func()