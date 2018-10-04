import * as React from "react";
import * as ReactDOM from "react-dom";

import { WsHandler, jsonrpcInit, jsonrpcCall } from '../utils/jsonrpc'

import { VolumeInfo, VolumesTab } from './volumes-tab'
import { DataTab } from './data-tab'
import { LookupTab } from './lookup-tab'
import { StoreTab } from './store-tab'

import { Tab, Loader, Grid } from 'semantic-ui-react'
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
        this.setState({ wsStatus: 'error' })
        alert("WebSocket error");
    }
    onWsDisconnect() {
        this.setState({ wsStatus: 'closed' })
        alert("WebSocket disconnected");
    }

    onError(reason:any)
    {
        console.log(reason)
        alert(reason)
    }
    onErrorBound = this.onError.bind(this)

    onReloadVolumes()
    {
        jsonrpcCall("get_volumes_list").then(volList => this.setVolumes(volList),this.onErrorBound)
    }
    onReloadVolumesBound = this.onReloadVolumes.bind(this)

    render() {

        let mainComponent;
        if(this.state.volumesLoaded) {
            /*
            const panes = [
                {
                    menuItem: 'Volumes',
                    pane: <Tab.Pane key='volumes'><VolumesTab volumes={this.state.volumes} reloadVolumes={this.onReloadVolumesBound}/></Tab.Pane>
                },
                {
                    menuItem: 'Store',
                    pane: <Tab.Pane key='store'><StoreTab /></Tab.Pane>
                },
                {
                    menuItem: 'Lookup',
                    pane: <Tab.Pane key='lookup'><LookupTab /></Tab.Pane>
                },
                {
                    menuItem: 'Browse',
                    pane: <Tab.Pane key='data'><DataTab /></Tab.Pane>
                },
            ]
            mainComponent = <Tab renderActiveOnly={false} panes={panes}></Tab>
            */
           mainComponent = <Grid columns={3}>
               <Grid.Row columns={1}>
                   <Grid.Column><VolumesTab volumes={this.state.volumes} reloadVolumes={this.onReloadVolumesBound}/></Grid.Column>
                </Grid.Row>
               <Grid.Row>
                   <Grid.Column><StoreTab/></Grid.Column>
                   <Grid.Column><LookupTab/></Grid.Column>
                   <Grid.Column><DataTab/></Grid.Column>
                </Grid.Row>
               </Grid>
        }
        else {
            mainComponent = <Loader active={true}>Loading...</Loader>;
        }

        return <div>
            {mainComponent}
        </div>
    }
}
