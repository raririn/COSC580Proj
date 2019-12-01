class PrintException:

    @staticmethod
    def dimensionError():
        print('Dimension not match.')
    
    @staticmethod
    def keyError(in_func = ''):
        print('Key not in dist! Exception in %s', in_func)

    @staticmethod
    def typeError():
        print('')

    @staticmethod
    def indexError():
        print('Index does not exist.')