
require('dotenv').config();
const mysql = require('mysql2/promise');

const RESET_HOUR = process.env.RESET_HOUR || 12;
const LIMITE_MIN = -0.1;
const LIMITE_MAX = 0.1;

async function atualizarGrafico() {
  const conn = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'senha123',
    database: 'finance_data'
  });

  const [ultimoTimestampRow] = await conn.execute(`
    SELECT MAX(timestamp) AS ultimo FROM ativos
  `);
  if (!ultimoTimestampRow[0].ultimo) {
    console.log('Nenhum dado encontrado na tabela ativos.');
    await conn.end();
    return;
  }
  const ultimoTimestamp = ultimoTimestampRow[0].ultimo;

  const [contagemAtual] = await conn.execute(`
    SELECT
      SUM(CASE WHEN sinal = 'Alta' THEN 1 ELSE 0 END) AS alta,
      SUM(CASE WHEN sinal = 'Queda' THEN 1 ELSE 0 END) AS queda,
      SUM(CASE WHEN sinal = 'Neutro' THEN 1 ELSE 0 END) AS neutro
    FROM ativos
    WHERE timestamp = ? AND tipo_contagem = 'simple'
  `, [ultimoTimestamp]);

   const [contagemTotal] = await conn.execute(`
    SELECT
      COUNT(*) as total
    FROM ativos
    WHERE timestamp = ? AND tipo_contagem = 'simple'
  `, [ultimoTimestamp]);

  const altaAtual = contagemAtual[0].alta || 0;
  const quedaAtual = contagemAtual[0].queda || 0;
  const neutroAtual = contagemAtual[0].neutro || 0;
  const totalAtivos = contagemTotal[0].total;

  const [ultimoHoje] = await conn.execute(`
    SELECT * FROM grafico
    WHERE DATE(timestamp) = CURDATE()
    ORDER BY timestamp DESC
    LIMIT 1
  `);

  let acumuladoAlta = 0, acumuladoQueda = 0, acumuladoNeutro = totalAtivos;

  if (ultimoHoje.length > 0) {
      const anterior = ultimoHoje[0];
      const rastroParcial = acumuladoAlta - acumuladoQueda;
      const rastroAnterior = ultimoHoje.length > 0 ? ultimoHoje[0].rastro_acumulado : 0;
      const rastroAcumulado = rastroAnterior + rastroParcial;

    await conn.execute(`
      INSERT INTO grafico (
        alta, queda, neutro,
        acumulado_alta, acumulado_queda, acumulado_neutro,
        rastro_parcial, rastro_acumulado,
        timestamp
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      altaAtual, quedaAtual, neutroAtual,
      acumuladoAlta, acumuladoQueda, acumuladoNeutro,
      rastroParcial, rastroAcumulado,
      new Date()
    ]);

    await conn.end();
  }

}

module.exports = { atualizarGrafico };
