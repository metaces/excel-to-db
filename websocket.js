const WebSocket = require('ws');
const mysql = require('mysql2/promise');

const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'senha123',
  database: 'finance_data'
};

const wss = new WebSocket.Server({ port: 8080 });

let enviadoHoje = false;
let ultimoTimestampEnviado = null;
let tipoAgregacao = 'media';
let dia = null;
let horaInicial = null;

wss.on('connection', async (ws) => {
  console.log('Cliente conectado ao WebSocket');

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      if (data.tipoAgregacao) {
        tipoAgregacao = data.tipoAgregacao;
        console.log(`Tipo de agregação alterado para: ${tipoAgregacao}`);
      }
      if (data.dia && data.horaInicial) {
        dia = data.dia;
        horaInicial = data.horaInicial;
        console.log(`Parâmetros recebidos: dia=${dia}, horaInicial=${horaInicial}`);
      }
    } catch (e) {
      console.error('Erro ao interpretar mensagem do cliente:', e);
    }
  });

  try {
    if (!dia || !horaInicial) {
      console.error('Parâmetros dia e horaInicial não foram enviados pelo cliente.');
      return;
    }

    const conn = await mysql.createConnection(dbConfig);
    const [rows] = await conn.execute(
      "SELECT * FROM grafico WHERE DATE(timestamp) = ? AND TIME(timestamp) >= ? ORDER BY timestamp ASC",
      [dia, horaInicial]
    );
    await conn.end();

    const agrupado = agruparPorIntervalo(rows, 5, tipoAgregacao);
    ultimoTimestampEnviado = agrupado[agrupado.length - 1].fim;
    enviadoHoje = true;

    ws.send(JSON.stringify({ tipo: 'full', dados: agrupado }));
    console.log('Enviado histórico inicial para novo cliente:', agrupado.length);
  } catch (err) {
    console.error('Erro ao enviar dados iniciais:', err);
  }
});

// Função para agrupar por intervalos fixos
function agruparPorIntervalo(rows, intervaloMinutos = 5, tipoAgregacao = 'media') {
  if (!rows || rows.length === 0) return [];
  const inicioDia = new Date(rows[0].timestamp);
  inicioDia.setSeconds(0, 0);
  const fimDia = new Date(rows[rows.length - 1].timestamp);
  const agrupado = [];
  let intervaloInicio = new Date(inicioDia);

  while (intervaloInicio <= fimDia) {
    const intervaloFim = new Date(intervaloInicio.getTime() + intervaloMinutos * 60000);
    const bloco = rows.filter(r => {
      const ts = new Date(r.timestamp);
      return ts >= intervaloInicio && ts < intervaloFim;
    });

    let agregado;
    if (bloco.length > 0) {
      agregado = calcularAgregacao(bloco, tipoAgregacao, intervaloInicio, intervaloFim);
    } else {
      agregado = {
        inicio: intervaloInicio.toISOString(),
        fim: intervaloFim.toISOString(),
        alta: null, queda: null, neutro: null,
        acumulado_alta: null, acumulado_queda: null, acumulado_neutro: null,
        rastro_parcial: null, rastro_acumulado: null
      };
    }
    agrupado.push(agregado);
    intervaloInicio = intervaloFim;
  }
  return agrupado;
}

// Função para calcular agregação
function calcularAgregacao(bloco, tipo, intervaloInicio = null, intervaloFim = null) {
  const soma = {
    alta: 0, queda: 0, neutro: 0,
    acumulado_alta: 0, acumulado_queda: 0, acumulado_neutro: 0,
    rastro_parcial: 0, rastro_acumulado: 0
  };

  bloco.forEach(r => {
    soma.alta += r.alta;
    soma.queda += r.queda;
    soma.neutro += r.neutro;
    soma.acumulado_alta += r.acumulado_alta;
    soma.acumulado_queda += r.acumulado_queda;
    soma.acumulado_neutro += r.acumulado_neutro;
    soma.rastro_parcial += r.rastro_parcial;
    soma.rastro_acumulado += r.rastro_acumulado;
  });

  const n = bloco.length;
  if (tipo === 'media') {
    for (let key in soma) soma[key] = (soma[key] / n).toFixed(2);
  }

  return {
    inicio: intervaloInicio ? intervaloInicio.toISOString() : bloco[0].timestamp,
    fim: intervaloFim ? intervaloFim.toISOString() : bloco[bloco.length - 1].timestamp,
    ...soma
  };
}

// Enviar dados a cada 5 minutos
setInterval(async () => {
  try {
    if (!dia || !horaInicial) return;

    const conn = await mysql.createConnection(dbConfig);
    const [rows] = await conn.execute(
      "SELECT * FROM grafico WHERE DATE(timestamp) = ? AND TIME(timestamp) >= ? ORDER BY timestamp ASC",
      [dia, horaInicial]
    );
    await conn.end();

    if (!enviadoHoje) {
      const agrupado = agruparPorIntervalo(rows, 5, tipoAgregacao);
      ultimoTimestampEnviado = agrupado[agrupado.length - 1].fim;
      enviadoHoje = true;
      wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify({ tipo: 'full', dados: agrupado }));
        }
      });
      console.log('Enviado todos os blocos do dia:', agrupado.length);
    } else {
      const novosRegistros = rows.filter(r => new Date(r.timestamp) > new Date(ultimoTimestampEnviado));
      if (novosRegistros.length > 0) {
        const intervaloInicio = new Date(ultimoTimestampEnviado);
        const intervaloFim = new Date(intervaloInicio.getTime() + 5 * 60000);
        const novoBloco = calcularAgregacao(novosRegistros, tipoAgregacao, intervaloInicio, intervaloFim);
        ultimoTimestampEnviado = novoBloco.fim;
        wss.clients.forEach(client => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ tipo: 'incremento', dados: novoBloco }));
          }
        });
        console.log('Enviado incremento:', novoBloco);
      }
    }
  } catch (err) {
    console.error('Erro ao enviar dados via WebSocket:', err);
  }
}, 5 * 60 * 1000);