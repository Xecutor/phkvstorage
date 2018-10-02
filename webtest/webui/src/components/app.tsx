import * as React from "react";
import * as ReactDOM from "react-dom";

import { WsHandler, jsonrpcInit, jsonrpcCall } from '../utils/jsonrpc'

import { VolumeInfo, VolumesTab } from './volumes-tab'
import { DataTab } from './data-tab'

import { Tab, Loader } from 'semantic-ui-react'
import 'semantic-ui-css/semantic.min.css';

interface PHKVSWebTestState {
    wsStatus:string,
    volumesLoaded:boolean
    volumes:VolumeInfo[]
}

export class PHKVSWebTest extends React.Component<any, PHKVSWebTestState> implements WsHandler {
    constructor(prop: any) {
        super(prop)
        this.state = {
            wsStatus:'Uninit',
            volumesLoaded:false,
            volumes:[]
        }
    }

    componentDidMount() {
        console.log('mount')
        jsonrpcInit(this)
    }

    setVolumes(volumes:VolumeInfo[])
    {
        this.setState({volumesLoaded:true, volumes});
    }

    onWsConnect() {
        this.setState({ wsStatus: 'open' })
        this.onReloadVolumes()
    }
    onWsError() {
        this.setState({ wsStatus: 'error' });
    }
    onWsDisconnect() {
        this.setState({ wsStatus: 'closed' });
    }

    onReloadVolumes()
    {
        jsonrpcCall("get_volumes_list").then(volList => this.setVolumes(volList))
    }
    onReloadVolumesBound = this.onReloadVolumes.bind(this)

    render() {

        let mainComponent;
        if(this.state.volumesLoaded) {
            const panes = [
                {
                    menuItem: 'Volumes',
                    pane: <Tab.Pane key='volumes'><VolumesTab volumes={this.state.volumes} reloadVolumes={this.onReloadVolumesBound}/></Tab.Pane>
                },
                {
                    menuItem: 'Data',
                    pane: <Tab.Pane key='data'><DataTab /></Tab.Pane>
                },
            ]
            mainComponent = <Tab renderActiveOnly={false} panes={panes}></Tab>
        }
        else {
            mainComponent = <Loader active={true}>Loading...</Loader>;
        }

        return <div>
            {mainComponent}
        </div>
    }
}
