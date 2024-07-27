function convertNumber(num) {
    if (typeof num !== 'number' || !Number.isInteger(num)) {
      throw new Error('El argumento debe ser un número entero.');
    }
    
    if (num % 2 === 0) {
      // Convertir a binario
      return num.toString(2);
    } else {
      // Convertir a hexadecimal
      return num.toString(16);
    }
  }