from flask import Flask, render_template, request, jsonify
import paramiko

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/execute', methods=['POST'])
def execute():
    data = request.get_json()
    ip = data.get('ip')
    command = data.get('command')
    username = data.get('username', 'admin')
    password = data.get('password', 'cisco')

    if not ip or not command:
        return jsonify({'error': 'Router IP and command are required.'}), 400

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        ssh.connect(
            hostname=ip,
            username=username,
            password=password,
            timeout=10,
            look_for_keys=False,
            allow_agent=False
        )
        
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode('utf-8', errors='ignore')
        error = stderr.read().decode('utf-8', errors='ignore')
        ssh.close()

        result = output if output else error
        return jsonify({'output': result if result else 'Command executed with no output.'})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
