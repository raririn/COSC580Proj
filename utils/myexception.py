import logging

class PrintException:

    @staticmethod
    def dimensionError():
        print('Dimension not match.')
    
    @staticmethod
    def keyError(in_func = ''):
        print('Key not in dist! Exception in %s', in_func)

    @staticmethod
    def typeError():
        print('t')

    @staticmethod
    def indexError():
        print('Index does not exist.')

    @staticmethod
    def syntaxError():
        print('s')

    @staticmethod
    def handlerError():
        logging.info('Handler can\'t execute the input.')