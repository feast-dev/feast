import subprocess

def port_forward(service, external_port, local_port=80) :
  """
  Run a background process to forward port 80 of the given `service` service to the given `external_port` port.

  Returns: the process instance
  """
  command = ["kubectl", "port-forward", f"service/{service}", f"{external_port}:{local_port}"]
  process = subprocess.Popen(command)
  print(f"Port-forwarding {service} with process ID: {process.pid}")
  return process
