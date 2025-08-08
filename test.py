print("printing from test file")

def test_func(a,b,c=None):
    print("printing from test function")

if __name__ == "__main__":
    test_func(1,2)

print("printing after test function")