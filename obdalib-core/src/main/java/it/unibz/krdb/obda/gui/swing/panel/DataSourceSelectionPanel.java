/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */
package it.unibz.krdb.obda.gui.swing.panel;

import it.unibz.krdb.obda.model.DataSource;
import it.unibz.krdb.obda.model.OBDAModel;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.net.URI;

import javax.swing.JOptionPane;

/**
 *
 * @author obda
 */
public class DataSourceSelectionPanel extends javax.swing.JPanel {

	/**
	 *
	 */
	private static final long		serialVersionUID	= -8124338850871507250L;
	private OBDAModel	dscontroller		= null;
	private DatasourceSelector		selector			= null;

	/** Creates new form DataSourceSelectionPanel */
	public DataSourceSelectionPanel(OBDAModel dscontroller) {

		this.dscontroller = dscontroller;
		initComponents();
		init();
//		setDatasourcesController(dscontroller);
	}

	/***
	 * Changes the datasource controller associated to this selector. This will
	 * remove and update listeners and reset the content of the combo box.
	 *
	 * @param dsc
	 */
	public void setDatasourcesController(OBDAModel dsc) {
		/* removing listeners and references to the old */
		dscontroller.removeSourcesListener(selector);
		dscontroller = dsc;

		selector.setDatasourceController(dsc);

		/* setup the new listener */
		dscontroller.addSourcesListener(selector);
	}

	public DatasourceSelector getDataSourceSelector() {
		return selector;
	}

	private void init() {

		selector = new DatasourceSelector(dscontroller);

		GridBagConstraints gridBagConstraints = new java.awt.GridBagConstraints();
		gridBagConstraints.gridx = 1;
		gridBagConstraints.gridy = 0;
		gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
		gridBagConstraints.weightx = 1.0;
		gridBagConstraints.insets = new java.awt.Insets(2, 5, 0, 0);
		add(selector, gridBagConstraints);
		dscontroller.addSourcesListener(selector);

		jButtonAdd.setText("Insert");
		jButtonAdd.setMnemonic('i');
		jButtonAdd.setToolTipText("Add a new datasource");
		jButtonAdd.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jButtonAdd.setContentAreaFilled(false);
		jButtonAdd.setIconTextGap(0);
		jButtonAdd.setMaximumSize(new java.awt.Dimension(60, 30));
		jButtonAdd.setMinimumSize(new java.awt.Dimension(50, 20));
		jButtonAdd.setPreferredSize(new java.awt.Dimension(50, 20));
		jButtonAdd.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent evt) {
				addButtonActionPerformed(evt);
			}
		});

		jButtonRemove.setText("Delete");
		jButtonRemove.setMnemonic('d');
		jButtonRemove.setToolTipText("Remove the selected datasource");
		jButtonRemove.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jButtonRemove.setContentAreaFilled(false);
		jButtonRemove.setIconTextGap(0);
		jButtonRemove.setMaximumSize(new java.awt.Dimension(60, 30));
		jButtonRemove.setMinimumSize(new java.awt.Dimension(50, 20));
		jButtonRemove.setPreferredSize(new java.awt.Dimension(50, 20));
		jButtonRemove.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent arg0) {
				DataSource ds = selector.getSelectedDataSource();
				if (ds != null) {
					dscontroller.removeSource(ds.getSourceID());
				}
			}
		});

		jLabeltitle.setBackground(new java.awt.Color(153, 153, 153));
		jLabeltitle.setFont(new java.awt.Font("Arial", 1, 11));
		jLabeltitle.setForeground(new java.awt.Color(153, 153, 153));
		jLabeltitle.setPreferredSize(new Dimension(119, 14));
	}

  private void addButtonActionPerformed(ActionEvent evt) {
    String name = JOptionPane.showInputDialog(
        "Insert the URI of the new data source.\n"
            + "The URI should be a unique idenfier for the data souce\n"
            + "not the connection url or the actual data source name.", null).trim();

    if (!name.isEmpty()) {
      URI uri = createUri(name);
      if (uri != null) {
        if (!dscontroller.containsSource(uri)) {
          dscontroller.addDataSource(name);
        }
        else {
          JOptionPane.showMessageDialog(this,
              "The data source ID already exists!", "Error",
              JOptionPane.ERROR_MESSAGE);
        }
      }
    }
  }

  private URI createUri(String name) {
    URI uri = null;
    try {
      uri = URI.create(name);
    }
    catch (IllegalArgumentException e) {
      JOptionPane.showMessageDialog(this,
          "Invalid name string!", "Error", JOptionPane.ERROR_MESSAGE);
    }
    return uri;
  }

	/**
	 * This method is called from within the constructor to initialize the form.
	 * WARNING: Do NOT modify this code. The content of this method is always
	 * regenerated by the Form Editor.
	 */
	@SuppressWarnings("unchecked")
	// <editor-fold defaultstate="collapsed"
	// desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {
		java.awt.GridBagConstraints gridBagConstraints;

		jButtonAdd = new javax.swing.JButton();
		jButtonRemove = new javax.swing.JButton();
		jLabeltitle = new javax.swing.JLabel();

		setBorder(javax.swing.BorderFactory.createTitledBorder("Datasource Selection"));
		setLayout(new java.awt.GridBagLayout());
		gridBagConstraints = new java.awt.GridBagConstraints();
		gridBagConstraints.gridx = 2;
		gridBagConstraints.gridy = 0;
		gridBagConstraints.insets = new Insets(5, 5, 5, 5);
		gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
		add(jButtonAdd, gridBagConstraints);
		gridBagConstraints = new java.awt.GridBagConstraints();
		gridBagConstraints.gridx = 3;
		gridBagConstraints.gridy = 0;
		gridBagConstraints.insets = new Insets(5, 5, 5, 5);
		gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
		add(jButtonRemove, gridBagConstraints);

		jLabeltitle.setText("Select datasource:");
		gridBagConstraints = new java.awt.GridBagConstraints();
		gridBagConstraints.gridx = 0;
		gridBagConstraints.gridy = 0;
		gridBagConstraints.gridwidth = 1;
		gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
		gridBagConstraints.anchor = java.awt.GridBagConstraints.WEST;
		gridBagConstraints.insets = new Insets(5, 5, 5, 5);
		add(jLabeltitle, gridBagConstraints);
		gridBagConstraints = new java.awt.GridBagConstraints();
	}// </editor-fold>//GEN-END:initComponents

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JButton	jButtonAdd;
	private javax.swing.JButton	jButtonRemove;
	private javax.swing.JLabel	jLabeltitle;
	// private javax.swing.JPanel jPanel1;
	// End of variables declaration//GEN-END:variables
}
