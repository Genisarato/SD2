import lithops
import boto3
import os 

# Dades d'entrada i llista d'insults
iterdata = ["s3://sdtask2/insults2.txt", "s3://sdtask2/insults.txt"]
insults = {"imbècil", "merda", "collons", "puto", "putades", "puta", "capullo"} 

def my_map_function(obj):
    s3_client = boto3.client('s3')
    
    # FIX: Access bucket and key directly from the CloudObject
    bucket = obj.bucket
    key = obj.key
    
    # Determinar la clau de sortida per a S3
    output_filename = key.split("/")[-1] 
    output_key = f'censored/{output_filename}'
    
    # Definir una ruta de fitxer temporal dins del directori /tmp/ de la funció Lambda
    temp_file_path = f'/tmp/censored_{output_filename}'
    
    total_censored_count = 0

    try:
        # Obrir el fitxer temporal per escriure contingut processat
        with open(temp_file_path, 'wb') as temp_output_file:
            # Processar el fitxer d'entrada línia per línia
            with obj.data_stream as stream:
                for line in stream:
                    decoded_line = line.decode('utf-8').strip()
                    words = []
                    line_censored_count = 0
                    for word in decoded_line.split():
                        if word.lower() in insults:
                            words.append("CENSORED")
                            line_censored_count += 1
                        else:
                            words.append(word)
                    total_censored_count += line_censored_count
                    
                    # Escriure la línia processada al fitxer temporal
                    temp_output_file.write((" ".join(words) + "\n").encode('utf-8'))
        
        # Pujar el fitxer temporal a S3
        s3_client.upload_file(
            Filename=temp_file_path,
            Bucket=bucket,
            Key=output_key
        )
        
    finally:
        # Netejar: eliminar el fitxer temporal per alliberar espai al disc
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

    return total_censored_count

def my_reduce_function(results):
    return sum(results)

if __name__ == "__main__":
    fexec = lithops.FunctionExecutor(
        backend='aws_lambda',
        runtime_memory=4096,   
        runtime_timeout=300,   
        log_level='INFO'       
    )
    
    future = fexec.map_reduce(
        map_function=my_map_function,
        map_iterdata=iterdata,
        reduce_function=my_reduce_function
    )
    
    result = fexec.get_result(future)
    print(f'Total insults censurats: {result}')
    
    fexec.clean()