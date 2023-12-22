from flask import Flask, request, jsonify
import os
from datetime import datetime
from scripts import meses_por_importe_total

app = Flask(__name__)

@app.route('/api/update/meses_por_importe_total', methods=['GET'])
def meses_por_importe_totales():
    if request.method == 'GET':
        print("Llamado a Función Meses Por Importe Total")
        result,bool = meses_por_importe_total.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull process meses_por_importe_total on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process meses_por_importe_total on {fecha_hoy}, details: {result}'})


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8089))
    app.run(debug=True, host='0.0.0.0', port=port)