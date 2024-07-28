class person:
    def __init__(self):
        self.name = "Yair Juarez"
        self.age = 20
        self.nation = "Argentina"
        
def convert_number(num):
    if num % 2 == 0:  # Si el número es par
        return bin(num)  # Convertir a binario
    else:  # Si el número es impar
        return hex(num)  # Convertir a hexadecimal
