insults = ['Gilipolles', 'Burro', 'Fill de puta']

def lambda_handler(event, context):
    mensaje = event['missatge_original']
    print(f"Missatge rebut per Lambda: {mensaje}")

    # Iterar sobre la llista d'insults
    for insult in insults:
        # Si l'insults està al missatge, el reemplaçem per 'CENSORED'
        if insult in mensaje:
            mensaje = mensaje.replace(insult, "CENSORED")
            print(f"Insulto trobat i reemplaçat: {insult}")

    # Mostrar el missatge
    print(f"Missatge filtrat: {mensaje}")

    # Retornem el processat
    return {
        'statusCode': 200,
        'body': f'Missatge processat correctament: {mensaje}'
    }
