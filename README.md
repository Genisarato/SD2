Task1 execution:
  Subscriber
    nohup python3 triggerLambda.py > logsTrigger.txt 2>&1 < /dev/null &
  Send messages
    python3 enviarmissatges.py 5000

Task2 execution:
  Manegador:
     python3 primitive_stream.py --function print_message --maxworkers 20 --queue primitive_stream > salida.log 2>&1 &
  Send messages:
  python3 enviarmissatges.py 5000

Task3 execution:
  Using PyRun for proper use of Lithops
