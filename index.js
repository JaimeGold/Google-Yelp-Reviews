function laCajaDePandora(numero) {
    if (numero % 2 === 0) {
        // El número es par, convertir a binario
        return numero.toString(2);
    } else {
        // El número es impar, convertir a hexadecimal
        return numero.toString(16);
    }
}

// Ejemplos de uso:
console.log(laCajaDePandora(10)); // Output: "1010" (binario)
console.log(laCajaDePandora(15)); // Output: "f" (hexadecimal)

function crearObjetoConDatos() {
    return {
        nombre: "Gustavo",
        edad: "23",
        nacionalidad: "Col"
    };
}

// Ejemplo de uso:
console.log(crearObjetoConDatos());
