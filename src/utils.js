let { Op } = require('sequelize');

const DESC_NULLS_FIRST = 'DESC NULLS FIRST'
const ASC_NULLS_FIRST = 'ASC NULLS FIRST'
const ASC_NULLS_LAST = 'ASC NULLS LAST'
const DESC_NULLS_LAST = 'DESC NULLS LAST'

if (!Op) {
  // Support older versions of sequelize
  Op = {
    and: '$and',
    or: '$or',
    lt: '$lt',
    lte: '$lte',
    gt: '$gt',
    gte: '$gte',
  };
}

const parseCursor = (cursor) => {
  if (!cursor) {
    return null;
  }

  try {
    return JSON.parse(Buffer.from(cursor, 'base64').toString('utf8'));
  } catch (e) {
    return null;
  }
};

const normalizePrimaryKeyField = (primaryKeyField) => {
  return Array.isArray(primaryKeyField) ? primaryKeyField : [primaryKeyField];
};

const ensurePrimaryKeyFieldInOrder = (order, primaryKeyField) => {
  const missingPrimaryKeyFields = primaryKeyField.filter(
    (pkField) => !order.find(([field]) => field === pkField),
  );

  return [...order, ...missingPrimaryKeyFields.map((field) => [field, 'ASC'])];
};

const normalizeOrder = (order, primaryKeyField, omitPrimaryKeyFromOrder) => {
  const normalizedPrimaryKeyField = normalizePrimaryKeyField(primaryKeyField);

  let normalized = [];

  if (Array.isArray(order)) {
    normalized = order.map((o) => {
      if (typeof o === 'string') {
        return [o, 'ASC'];
      }

      if (Array.isArray(o)) {
        const directionValueIndex = o.length - 1;
        o[directionValueIndex] = o[directionValueIndex] || 'ASC';

        return o;
      }

      return o;
    });
  }

  return omitPrimaryKeyFromOrder
    ? normalized
    : ensurePrimaryKeyFieldInOrder(normalized, normalizedPrimaryKeyField);
};

const reverseOrder = (order, enforceNullOrder) => {
  return order.map((orderItem) => {
    const keyIndexToUpdate = orderItem.length - 1

    // reverse order for DESC_NULLS_LAST should be ASC_NULLS_FIRST
    if(orderItem[keyIndexToUpdate].toUpperCase() === DESC_NULLS_LAST) {
      orderItem[keyIndexToUpdate] = ASC_NULLS_FIRST
      return orderItem;
    }
    if(orderItem[keyIndexToUpdate].toLowerCase().split(' ')[0] === 'desc') {
      orderItem[keyIndexToUpdate] = enforceNullOrder ? ASC_NULLS_LAST : 'ASC'
      return orderItem;
    }
    orderItem[keyIndexToUpdate] = enforceNullOrder ? DESC_NULLS_FIRST : 'DESC'
    return orderItem;
  });
};

const serializeCursor = (payload) => {
  return Buffer.from(JSON.stringify(payload)).toString('base64');
};

const getFieldValue = (instance, orderItem) => {
  try {
    const fieldValue = instance[orderItem[0]['as']][orderItem[1]]
    if(!fieldValue) {
      return instance['dataValues'][orderItem[0]['as']][0]['dataValues'][orderItem[1]]
    } else {
      return fieldValue
    }
  } catch(e) {
    return null
  }
}

const createCursor = (instance, order) => {
  // order
  // const fields
  const payload = order.map((orderItem) => {
    let field;
    if (typeof orderItem[0] == 'object') {
      return getFieldValue(instance, orderItem)
    } else {
      field = orderItem[0];
    }

    return instance[field];
  });

  return serializeCursor(payload);
};

const isValidCursor = (cursor, order) => {
  return cursor.length === order.length;
};

const recursivelyGetPaginationQuery = (order, cursor) => {
  const directionValueIndex = order[0].length - 1;

  const orderValue = order[0][directionValueIndex]
  const currentOp = orderValue.toLowerCase().split(' ')[0] === 'desc' ? Op.lt : Op.gt;
  
  let operatorFilter = { [currentOp]: cursor[0] }

  // supporting only below format
  // [
  //   { model: Task, as: 'Task' },
  //   { model: Project, as: 'Project' },
  //   'createdAt',
  //   'DESC',
  // ];

  const _getColumnName = (order) => {
    // check if we have json object

    if (typeof order[0][0] === 'object') {
      const name = `${order[0][0]['as']}.${order[0][1]}`;
      return '$' + name + `$`;
    } else {
      return order[0][0];
    }
  };

  if (order.length === 1) {
    const key = _getColumnName(order);

    return {
      [key]: {
        [currentOp]: cursor[0],
      },
    };
  } else {
    const key = _getColumnName(order);

    // https://github.com/goSprinto/sequelize-cursor-pagination#ordering-columns-with-null-values
    if(cursor[0] === null &&  [DESC_NULLS_FIRST, ASC_NULLS_FIRST].includes(orderValue)) {
      operatorFilter = { [Op.ne]: cursor[0] }
    }
  
    if(cursor[0] !== null && [DESC_NULLS_LAST,ASC_NULLS_LAST].includes(orderValue)) {
      operatorFilter = {[Op.or]: [{[Op.is]: null}, operatorFilter] }
    }

    return {
      [Op.or]: [
        { [key]: operatorFilter},
        {
          [key]: cursor[0],
          ...recursivelyGetPaginationQuery(order.slice(1), cursor.slice(1)),
        },
      ],
    };
  }
};

const getPaginationQuery = (order, cursor) => {
  if (!isValidCursor(cursor, order)) {
    return null;
  }

  return recursivelyGetPaginationQuery(order, cursor);
};

module.exports = {
  Op,
  parseCursor,
  serializeCursor,
  normalizeOrder,
  isValidCursor,
  getPaginationQuery,
  createCursor,
  reverseOrder,
};
