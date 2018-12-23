import React from 'react';
import { Form, Input, Select, Dialog, ConfigProvider } from '@alifd/next';
import { connect } from 'react-redux';
import { getTypes, add } from '../../reducers/cluster';
import '../../style/dialog-form.scss';

const FormItem = Form.Item;
const { Option } = Select;

@connect(state => ({ ...state.cluster }), { getTypes }, null, { withRef: true })
@ConfigProvider.config
class AddConfigDialog extends React.Component {
    static displayName = 'AddConfigDialog'

    constructor(props) {
      super(props);
      this.state = {
        visible: false,
        clusterName: '',
        clusterType: '',
        connectKeyList: [],
      };
    }

    componentDidMount() {
      this.props.getTypes();
    }

    save() {
      const { clusterName, clusterType, connectKeyList } = this.state;
      add({ clusterName, clusterType, connectKeyList })
        .then(() => {
          this.props.turnPage(1);
          this.close();
        })
        .catch(() => this.close());
    }

    close() {
      this.setState({ visible: false });
    }

    open = () => this.setState({ visible: true })

    render() {
      const { types = [], locale = {} } = this.props;
      return (
        <Dialog
          className="dialog-form"
          title={locale.title}
          visible={this.state.visible}
          onOk={() => this.save()}
          onCancel={() => this.close()}
          onClose={() => this.close()}
        >
          <Form>
            <FormItem label={`${locale.clusterName}:`}>
              <Input
                onChange={clusterName => this.setState({ clusterName })}
                placeholder={locale.clusterNamePlaceholder}
              />
            </FormItem>
            <FormItem label={`${locale.clusterType}:`}>
              <Select
                onChange={clusterType => this.setState({ clusterType })}
              >
                {
                                types.map(type => (
                                  <Option value={type} key={type}>{type}</Option>
                                ))
                            }
              </Select>
            </FormItem>
            <FormItem label={`${locale.connectKeyList}:`}>
              <Input.TextArea
                onChange={(connectKeyListStr) => {
                  this.setState({ connectKeyList: connectKeyListStr.split('\n') });
                }}
                placeholder={locale.connectKeyListPlaceholder}
              />
            </FormItem>
          </Form>
        </Dialog>
      );
    }
}

export default AddConfigDialog;
