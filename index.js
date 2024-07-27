function laCajaDePandora(numero) {
    if (numero % 2 === 0) {
        // Convertir a binario y retornar
        return numero.toString(2);
    } else {
        // Convertir a hexadecimal y retornar
        return numero.toString(16);
    }
}

// Ejemplos de uso
console.log(laCajaDePandora(4)); // "100" (4 en binario)
console.log(laCajaDePandora(7)); // "7" (7 en hexadecimal)