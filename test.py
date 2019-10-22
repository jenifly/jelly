class tt:
    a = 'dsd'

    @classmethod
    def m(cls):
        print(cls.a)
t = tt()
tt.a = 'mm'
tt.m()