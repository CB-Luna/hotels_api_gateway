from flask import Flask, request, jsonify
import os
from datetime import datetime
from scripts import sum_total_indicators

app = Flask(__name__)

@app.route('/api/update/sum_total_indicators/<int:document_id>', methods=['GET'])
def sum_total_indicator(document_id):
    if request.method == 'GET':
        print("Calling to Sum Total Indicators")
        result,bool = sum_total_indicators.main(document_id)
        # Getting date today
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull process sum_total_indicators on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process sum_total_indicators on {fecha_hoy}, details: {result}'})

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8089))
    app.run(debug=True, host='0.0.0.0', port=port)