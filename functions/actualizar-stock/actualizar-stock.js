import fetch from 'node-fetch';

export async function handler(event, context) {
  const STORE = process.env.SHOPIFY_STORE;
  const TOKEN = process.env.SHOPIFY_API_KEY;
  const VERSION = process.env.SHOPIFY_API_VERSION;

  const today = new Date().toISOString().split('T')[0];
  const actualizados = [];

  // Paso 1: Obtener productos (paginado con límite 50 por simplicidad)
  const res = await fetch(`${STORE}/admin/api/${VERSION}/products.json?limit=50`, {
    headers: {
      'X-Shopify-Access-Token': TOKEN,
      'Content-Type': 'application/json'
    }
  });

  const { products } = await res.json();

  for (const product of products) {
    const productId = product.id;

    // Paso 2: Obtener los 2 metafields personalizados del producto
    const metaRes = await fetch(`${STORE}/admin/api/${VERSION}/products/${productId}/metafields.json`, {
      headers: {
        'X-Shopify-Access-Token': TOKEN,
        'Content-Type': 'application/json'
      }
    });

    const { metafields } = await metaRes.json();

    const fechaMeta = metafields.find(m => m.namespace === 'custom' && m.key === 'fecha_disponibilidad');
    const stockMeta = metafields.find(m => m.namespace === 'custom' && m.key === 'stock_programado');

    if (!fechaMeta || !stockMeta) continue;

    const fechaDisponible = fechaMeta.value; // formato ISO
    const cantidadStock = parseInt(stockMeta.value || '0', 10);

    // Si la fecha es hoy y hay cantidad, actualizamos
    if (fechaDisponible === today && cantidadStock > 0) {
      const variantId = product.variants[0].id;

      // Actualizar inventario
      const updateRes = await fetch(`${STORE}/admin/api/${VERSION}/variants/${variantId}.json`, {
        method: 'PUT',
        headers: {
          'X-Shopify-Access-Token': TOKEN,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          variant: {
            id: variantId,
            inventory_quantity: cantidadStock,
            inventory_management: 'shopify'
          }
        })
      });

      actualizados.push({
        productId,
        variantId,
        fechaDisponible,
        cantidadStock
      });
    }
  }

  return {
    statusCode: 200,
    body: JSON.stringify({
      message: 'Proceso completado',
      actualizados
    })
  };
}
