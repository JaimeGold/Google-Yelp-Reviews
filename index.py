def la_caja_de_pandora(numero):
    if numero % 2 == 0:
        # Convertir el número a binario y retornarlo
        return bin(numero)
    else:
        # Convertir el número a hexadecimal y retornarlo
        return hex(numero)