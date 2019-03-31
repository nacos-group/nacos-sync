import request from '../utils/request';
import { CLUSTER_LIST, GET_CLUSTER_BY_ID, CLUSTER_TYPES, PAGE_SIZE } from '../constants';

const PATH = '/v1/cluster/';
const initialState = {
  totalPage: 0,
  totalSize: 0,
  clusterModel: {},
  clusterModels: [],
  types: [],
};

const add = data => request.post(`${PATH}add`, data);

const deleteCluster = params => request.delete(`${PATH}delete`, { params });

const list = params => dispatch => request.get(`${PATH}list`, {
  params: {
    pageSize: PAGE_SIZE,
    ...params,
  },
}).then(data => dispatch({
  type: CLUSTER_LIST,
  data,
}));

const detail = params => dispatch => request.get(`${PATH}detail`, { params }).then(({ clusterModel }) => dispatch({
  type: GET_CLUSTER_BY_ID,
  data: clusterModel,
}));

const getTypes = () => dispatch => request.get(`${PATH}types`).then(({ types }) => dispatch({
  type: CLUSTER_TYPES,
  data: types,
}));

export default (state = initialState, action) => {
  switch (action.type) {
    case CLUSTER_LIST:
      return { ...state, ...action.data };
    case GET_CLUSTER_BY_ID:
      return { ...state, clusterModel: action.data };
    case CLUSTER_TYPES:
      return { ...state, types: action.data };
    default:
      return state;
  }
};

export { list, add, deleteCluster, detail, getTypes };
